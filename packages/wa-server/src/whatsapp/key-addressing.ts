export type BaileysAddressingKey = {
  remoteJid?: string | null;
  participant?: string | null;
  remoteJidAlt?: string | null;
  participantAlt?: string | null;
  senderPn?: string | null;
  senderLid?: string | null;
  participantPn?: string | null;
  participantLid?: string | null;
};

export function resolveKeyAddressing(key: BaileysAddressingKey | undefined | null) {
  const remoteJid = key?.remoteJid ?? null;
  const participant = key?.participant ?? null;

  const senderPn =
    key?.senderPn ??
    (remoteJid?.endsWith("@lid") ? key?.remoteJidAlt ?? null : remoteJid);
  const senderLid =
    key?.senderLid ?? (remoteJid?.endsWith("@lid") ? remoteJid : null);
  const participantPn =
    key?.participantPn ??
    key?.participantAlt ??
    (participant?.endsWith("@lid") ? null : participant);
  const participantLid =
    key?.participantLid ?? (participant?.endsWith("@lid") ? participant : null);

  return {
    senderPn,
    senderLid,
    participantPn,
    participantLid,
    senderJid: senderPn ?? remoteJid ?? "",
    sender: participantPn ?? participant ?? remoteJid ?? "",
  };
}
