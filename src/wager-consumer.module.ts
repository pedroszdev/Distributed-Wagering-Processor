import { Module } from "@nestjs/common";
import { SqsWagerTransactionConsumer } from "./infrastructure/messaging/sqs-wager-transaction.consumer.ts";
import { WageringModule } from "./wagering.module.ts";

/**
 * Entrada pela FILA. Importa o WageringModule para usar o MESMO
 * ProcessWagerTransactionUseCase da entrada HTTP.
 */
@Module({
  imports: [WageringModule],
  providers: [SqsWagerTransactionConsumer],
  exports: [SqsWagerTransactionConsumer], // os testes chamam pollOnce() na mão
})
export class WagerConsumerModule {}
