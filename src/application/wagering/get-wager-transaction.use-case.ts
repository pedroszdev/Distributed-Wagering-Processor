import { EntityManager } from "@mikro-orm/postgresql";
import { WagerTransactionRepository } from "../../infrastructure/database/repositories/wager-transaction.repository.ts";
import { WagerTransaction } from "../../domain/wager-transaction/wager-transaction.ts";
import { WagerTransactionNotFoundError } from "./wagering.errors.ts";
import { Injectable } from "@nestjs/common";

@Injectable()
export class GetWagerTransactionUseCase {
  constructor(
    private readonly em: EntityManager,
    private readonly transactions: WagerTransactionRepository,
  ) {}

  async byId(transactionId: string): Promise<WagerTransaction> {
    const response = await this.transactions.findById(this.em.fork(), transactionId)
    if (!response) {
        throw new WagerTransactionNotFoundError(transactionId)
    }
    return response
  }

    async byExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction> {
        const response = await this.transactions.findByExternalId(this.em.fork(), providerId, externalTransactionId)
        if (!response) {
          throw new WagerTransactionNotFoundError(`${externalTransactionId} do provider ${providerId}`)
        }
        return response
    }
}
