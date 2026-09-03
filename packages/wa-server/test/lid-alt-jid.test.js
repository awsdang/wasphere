const { test } = require('node:test');
const assert = require('node:assert');
const { sanitizeMessage } = require('../dist/webhooks/sanitize-message');

test('sanitizeMessage prefers Baileys v7 remoteJidAlt / participantAlt over opaque @lid IDs', () => {
  const msg = {
    key: {
      remoteJid: '268401264046103@lid',
      remoteJidAlt: '268401264046103@s.whatsapp.net',
      participant: '987654321@lid',
      participantAlt: '987654321@s.whatsapp.net',
      fromMe: false,
      id: 'msg-1',
    },
    messageTimestamp: 1700000000,
    pushName: 'Alice',
  };

  const out = sanitizeMessage(msg);
  assert.equal(out.key.senderPn, '268401264046103@s.whatsapp.net');
  assert.equal(out.key.senderLid, '268401264046103@lid');
  assert.equal(out.key.participantPn, '987654321@s.whatsapp.net');
  assert.equal(out.key.participantLid, '987654321@lid');
  assert.equal(out.key.remoteJid, '268401264046103@lid');
});
