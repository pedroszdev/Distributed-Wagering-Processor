import { Body, Controller, Get, HttpCode, HttpStatus, Param, ParseUUIDPipe, Post, Query } from "@nestjs/common";
import { InvalidPayloadError } from "../application/shared/invalid-payload.error.ts";
import { CreateWalletUseCase } from "../application/wallet/create-wallet.use-case.ts";
import { GetWalletUseCase } from "../application/wallet/get-wallet.use-case.ts";
import { parseCreateWalletRequest } from "./create-wallet.request.ts";
import { toWalletResponse, type WalletResponse } from "./wallet.response.ts";
import { LedgerPageResponse, toLedgerPageResponse } from "./ledger.response.ts";
import { ListWalletLedgerUseCase } from "../application/wallet/list-wallet-ledger.use-case.ts";
import { parseLedgerQuery } from "./ledger.request.ts";
import { ReconcileWalletUseCase, type ReconciliationReport } from "../application/wallet/reconcile-wallet.use-case.ts";

/** Pipe do Nest para o :walletId, lançando o NOSSO erro (mesmo formato de resposta). */
const walletIdPipe = new ParseUUIDPipe({
  exceptionFactory: () => new InvalidPayloadError("walletId deve ser um UUID"),
});

/**
 * Controller "magro": só traduz HTTP <-> use case. Nenhuma regra de negócio aqui,
 * para que o consumidor SQS possa reaproveitar os mesmos use cases.
 */
@Controller("wallets")
export class WalletsController {
  constructor(
    private readonly createWallet: CreateWalletUseCase,
    private readonly getWallet: GetWalletUseCase,
    private readonly listLedger: ListWalletLedgerUseCase,
    private readonly reconcileWallet: ReconcileWalletUseCase,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async create(@Body() body: unknown): Promise<WalletResponse> {
    const input = parseCreateWalletRequest(body);
    const wallet = await this.createWallet.execute(input);
    return toWalletResponse(wallet);
  }

  @Get(":walletId")
  async get(@Param("walletId", walletIdPipe) walletId: string): Promise<WalletResponse> {
    const wallet = await this.getWallet.execute(walletId);
    return toWalletResponse(wallet);
  }

  @Get(":walletId/ledger")
  async ledger(
    @Param("walletId", walletIdPipe) walletId: string,
    @Query("cursor") cursor: string | undefined,
    @Query("limit") limit: string | undefined,
  ): Promise<LedgerPageResponse> {
    const page   = parseLedgerQuery(cursor, limit)
    const result = await this.listLedger.execute(walletId, page)
    return toLedgerPageResponse(walletId, result)
  }

  /**
   * POST (e não GET) porque é uma AÇÃO com efeito: registra métrica e, se
   * divergir, um log de erro. 200 nos dois casos: a divergência vem no body
   * (`consistent: false`), não é falha da requisição.
   */
  @Post(":walletId/reconciliation")
  @HttpCode(HttpStatus.OK)
  async reconcile(@Param("walletId", walletIdPipe) walletId: string): Promise<ReconciliationReport> {
    return this.reconcileWallet.execute(walletId);
  }
}
