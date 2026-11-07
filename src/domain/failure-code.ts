export enum FailureCode {
  InsufficientFunds = 'INSUFFICIENT_FUNDS',            // BET sem saldo
  ReversalWouldOverdraw = 'REVERSAL_WOULD_OVERDRAW',    // reversão deixaria saldo negativo
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',            // referência nunca apareceu (esgotou tentativas)
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',    // referência foi rejeitada/falhou
  ReferenceMismatch = 'REFERENCE_MISMATCH',             // provider/player/wallet/rodada diferentes
  ReferenceKindInvalid = 'REFERENCE_KIND_INVALID',      // tipo de referência não permitido
  AlreadyReversed = 'ALREADY_REVERSED',                 // referência já revertida
  AmountMismatch = 'AMOUNT_MISMATCH',                   // valor diferente da referência
  CurrencyMismatch = 'CURRENCY_MISMATCH',               // moeda diferente da wallet/referência
  PlayerMismatch = 'PLAYER_MISMATCH',                   // playerId não é dono da wallet
}