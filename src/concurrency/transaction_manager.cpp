//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_ref->read_ts_ = last_commit_ts_.load();
  txn_ref->commit_ts_ = INVALID_TS;
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  if (txn->state_ == TransactionState::TAINTED) {
    return false;
  }
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  const auto commit_ts = last_commit_ts_.load() + 1;

  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    if (table_info == nullptr) {
      continue;
    }
    auto table_heap = table_info->table_.get();
    for (const auto &rid : rids) {
      auto meta = table_heap->GetTupleMeta(rid);
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        meta.ts_ = commit_ts;
        table_heap->UpdateTupleMeta(meta, rid);
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->commit_ts_ = commit_ts;
  last_commit_ts_.store(commit_ts);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    if (table_info == nullptr) {
      continue;
    }
    auto table_heap = table_info->table_.get();
    for (const auto &rid : rids) {
      auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table_heap, rid);
      if (meta.ts_ != txn->GetTransactionTempTs()) {
        continue;
      }

      if (undo_link.has_value() && undo_link->prev_txn_ == txn->GetTransactionId()) {
        auto log = txn->GetUndoLog(undo_link->prev_log_idx_);
        auto prev_link = log.prev_version_.IsValid() ? std::optional<UndoLink>{log.prev_version_} : std::nullopt;
        auto restore_meta = TupleMeta{log.ts_, log.is_deleted_};
        auto restore_tuple = log.tuple_;
        static_cast<void>(UpdateTupleAndUndoLink(
            this, rid, prev_link, table_heap, txn, restore_meta, restore_tuple,
            [temp_ts = txn->GetTransactionTempTs()](const TupleMeta &curr_meta, const Tuple &, RID,
                                                    std::optional<UndoLink>) { return curr_meta.ts_ == temp_ts; }));
      } else {
        auto abort_meta = TupleMeta{0, true};
        static_cast<void>(UpdateTupleAndUndoLink(
            this, rid, std::nullopt, table_heap, txn, abort_meta, tuple,
            [temp_ts = txn->GetTransactionTempTs()](const TupleMeta &curr_meta, const Tuple &, RID,
                                                    std::optional<UndoLink>) { return curr_meta.ts_ == temp_ts; }));
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  const auto watermark = running_txns_.GetWatermark();

  std::vector<txn_id_t> to_remove;
  to_remove.reserve(txn_map_.size());

  for (const auto &[txn_id, txn] : txn_map_) {
    const auto state = txn->GetTransactionState();
    if (state == TransactionState::RUNNING || state == TransactionState::TAINTED) {
      continue;
    }

    if (state == TransactionState::ABORTED) {
      to_remove.push_back(txn_id);
      continue;
    }

    if (txn->GetUndoLogNum() == 0 || txn->GetCommitTs() <= watermark) {
      to_remove.push_back(txn_id);
    }
  }

  for (const auto &txn_id : to_remove) {
    txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
