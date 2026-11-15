import { DomainError } from "../../domain/shared/domain-error.ts";

/** Sem credencial válida → 401. */
export class UnauthenticatedError extends DomainError {
  readonly code = "UNAUTHENTICATED";

  constructor(reason: string) {
    super(`Não autenticado: ${reason}`);
  }
}

/** O provedor autenticado tentou agir em nome de OUTRO provedor → 403. */
export class ProviderMismatchError extends DomainError {
  readonly code = "PROVIDER_MISMATCH";

  constructor(
    readonly authenticatedProviderId: string,
    readonly requestedProviderId: string,
  ) {
    super(`O provedor autenticado (${authenticatedProviderId}) não pode operar como ${requestedProviderId}`);
  }
}
