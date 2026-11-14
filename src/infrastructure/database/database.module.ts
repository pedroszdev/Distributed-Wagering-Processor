import { Module } from "@nestjs/common";
import { WalletRepository } from "./repositories/wallet.repository.ts";
import { WagerTransactionRepository } from "./repositories/wager-transaction.repository.ts";
import { LedgerEntryRepository } from "./repositories/ledger-entry.repository.ts";
import { OutboxMessageRepository } from "./repositories/outbox-message.repository.ts";

@Module({
  providers: [WalletRepository, WagerTransactionRepository, LedgerEntryRepository, OutboxMessageRepository],
  exports: [WalletRepository, WagerTransactionRepository, LedgerEntryRepository, OutboxMessageRepository],
})
export class DatabaseModule {}
