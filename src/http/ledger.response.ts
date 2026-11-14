import type { LedgerPage } from "../application/wallet/list-wallet-ledger.use-case.ts";
import type { LedgerDirection } from "../domain/ledger/ledger-direction.ts";
import type { MoneyProps } from "../domain/money/money.ts";
import { encodeLedgerCursor } from "./ledger.request.ts";

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string; // ISO
}

export interface LedgerPageResponse {
  walletId: string;
  items: LedgerEntryView[];
  nextCursor: string | null;
}

export function toLedgerPageResponse(walletId: string, page: LedgerPage): LedgerPageResponse {
  return {
    walletId,
    items: page.entries.map((entry) => ({
        id: entry.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        createdAt: entry.createdAt.toISOString()
    })),
    nextCursor: page.nextAfterId ? encodeLedgerCursor(page.nextAfterId) : null
  };
}
