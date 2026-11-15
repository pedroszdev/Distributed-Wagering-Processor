import { Module } from "@nestjs/common";
import { WalletRepository } from "./repositories/wallet.repository.ts";
import { WagerTransactionRepository } from "./repositories/wager-transaction.repository.ts";
import { LedgerEntryRepository } from "./repositories/ledger-entry.repository.ts";
import { OutboxMessageRepository } from "./repositories/outbox-message.repository.ts";
import { InboxMessageRepository } from "./repositories/inbox-message.repository.ts";

const REPOSITORIES = [
  WalletRepository,
  WagerTransactionRepository,
  LedgerEntryRepository,
  OutboxMessageRepository,
  InboxMessageRepository,
];

@Module({
  providers: REPOSITORIES,
  exports: REPOSITORIES,
})
export class DatabaseModule {}
