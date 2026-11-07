export class DomainError extends Error {
  constructor(public readonly code: string, message: string) {
    super(message);
  }
}
export class ValidationError extends DomainError {
  constructor(m: string) { super('VALIDATION_ERROR', m); }
}
export class InvalidTransactionStateError extends DomainError {
  constructor(m: string) { super('INVALID_TRANSACTION_STATE', m); }
}
export class InsufficientFundsError extends DomainError {
  constructor() { super('INSUFFICIENT_FUNDS', 'insufficient funds'); }
}
export class CurrencyMismatchError extends DomainError {
  constructor(a: string, b: string) { super('CURRENCY_MISMATCH', 'currency mismatch: ' + a + ' vs ' + b); }
}
export class IdempotencyConflictError extends DomainError {
  constructor() { super('IDEMPOTENCY_CONFLICT', 'same key with different payload'); }
}
export class WalletNotFoundError extends DomainError {
  constructor() { super('WALLET_NOT_FOUND', 'wallet not found'); }
}
export class WalletAlreadyExistsError extends DomainError {
  constructor() { super('WALLET_ALREADY_EXISTS', 'wallet already exists for player and currency'); }
}
/** Falha de infraestrutura que vale a pena tentar de novo. */
export class TransientError extends Error {}