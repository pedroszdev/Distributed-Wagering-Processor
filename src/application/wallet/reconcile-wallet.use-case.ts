import { EntityManager } from "@mikro-orm/postgresql";
import { Injectable, Logger } from "@nestjs/common";
import { Money, type MoneyProps } from "../../domain/money/money.ts";
import { LedgerEntryRepository } from "../../infrastructure/database/repositories/ledger-entry.repository.ts";
import { Metrics } from "../../infrastructure/observability/metrics.ts";
import { WalletNotFoundError } from "./wallet.errors.ts";

/** O formato da resposta da seção 9 do desafio. */
export interface ReconciliationReport {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps; // stored − calculated (negativo = a wallet tem MENOS que o ledger)
  consistent: boolean;
  checkedEntries: number;
}

/**
 * Reconciliação: o saldo gravado na wallet bate com o saldo reconstruído pelo
 * ledger (soma dos créditos − soma dos débitos)?
 *
 * Divergência NUNCA é corrigida aqui (seção 9): corrigir esconderia o bug que
 * a causou. Ela é logada como erro, contada na métrica e sinalizada na resposta,
 * para alguém investigar.
 */
@Injectable()
export class ReconcileWalletUseCase {
  private readonly logger = new Logger(ReconcileWalletUseCase.name);

  constructor(
    private readonly em: EntityManager,
    private readonly ledger: LedgerEntryRepository,
    private readonly metrics: Metrics,
  ) {}

  async execute(walletId: string): Promise<ReconciliationReport> {
    const snapshot = await this.ledger.reconciliationSnapshot(this.em.fork(), walletId);
    if (!snapshot) throw new WalletNotFoundError(walletId);

    const money = (amount: string) => Money.from({ amount, currency: snapshot.currency });
    const stored = money(snapshot.storedAmount);
    const calculated = money(snapshot.creditsAmount).subtract(money(snapshot.debitsAmount));
    const difference = stored.subtract(calculated);
    const consistent = difference.isZero();

    const report: ReconciliationReport = {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: snapshot.entries,
    };

    this.metrics.walletReconciliations.inc({ result: consistent ? "consistent" : "divergent" });
    if (!consistent) {
      this.logger.error({ msg: "reconciliação DIVERGENTE: saldo da wallet ≠ ledger (não corrigido)", ...report });
    }
    return report;
  }
}
