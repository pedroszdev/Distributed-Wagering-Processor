import { HttpStatus } from "@nestjs/common";
import type { WagerTransactionResult } from "../application/wagering/process-wager-transaction.use-case.ts";
import { WagerTransactionStatus } from "../domain/wager-transaction/wager-transaction.enums.ts";

/** O status HTTP de um resultado (a tabela da decisão acima). */
export function httpStatusFor(result: WagerTransactionResult): HttpStatus {
    switch (result.status) {
        case(WagerTransactionStatus.Processed): {
            if (!result.idempotentReplay) {
                return HttpStatus.CREATED
            }
            return HttpStatus.OK
        }
        case(WagerTransactionStatus.PendingReference): {
            return HttpStatus.ACCEPTED
        }
        case(WagerTransactionStatus.Rejected): {
            return HttpStatus.UNPROCESSABLE_ENTITY
        }
        default:
            return HttpStatus.INTERNAL_SERVER_ERROR
    }
}
