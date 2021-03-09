// @ts-ignore
import syncProtocol from 'y-protocols/dist/sync.cjs'
// @ts-ignore
import encoding from 'lib0/dist/encoding.cjs'
// @ts-ignore
import decoding from 'lib0/dist/decoding.cjs'
import { addConnection, getConnection, getConnectionIds, removeConnection, getOrCreateDoc, updateDoc } from '../db/aws.js'
import ws from 'aws-lambda-ws-server'
// @ts-ignore
import { toBase64, fromBase64 } from 'lib0/dist/buffer.cjs'

const messageSync = 0
const messageAwareness = 1

const getDocName = (event) => {
  const qs = event.multiValueQueryStringParameters

  if (!qs || !qs.doc) {
    throw new Error('must specify ?doc=DOC_NAME')
  }

  return qs.doc[0]
}

const bufferedMessage = 69
const lastBufferedMessage = 70
const splitIntoBufferedMessage = (arr, size) => {
  const numChunks = Math.ceil(arr.length / size)
  const chunks = new Array(numChunks)

  for (let i = 0, o = 0; i < numChunks; ++i, o += size) {
    const isLast = i === numChunks - 1
    const encoder = encoding.createEncoder()
    encoding.writeUint8(encoder, isLast ? lastBufferedMessage : bufferedMessage)
    encoding.writeVarUint8Array(encoder, arr.slice(o, o + size))
    chunks[i] = toBase64(encoding.toUint8Array(encoder))
  }

  return chunks
}

const send = async ({ context, message, id }) => {
  const MESSAGE_MAX_CHUNK_SIZE_IN_BYTES = parseInt(process.env.MESSAGE_MAX_CHUNK_SIZE_IN_BYTES)
  console.log("MICHAL: message length", message.length);
    if(message.length > MESSAGE_MAX_CHUNK_SIZE_IN_BYTES) {
      // Dividing by 1.43 to take into account the chunk size
      // increase when converting from Uint8array to base64. It increases by ~33%, took extra 10% buffer just in case
      const chunks = splitIntoBufferedMessage(message, MESSAGE_MAX_CHUNK_SIZE_IN_BYTES / 1.43)
      for(let i=0; i < chunks.length; i++) {
          await context.postToConnection(chunks[i], id)
              .catch((err) => {
                  console.error(`Error during postToConnection: ${err}`)
                  return removeConnection(id)
              })
      }
    } else {
        return context.postToConnection(toBase64(message), id)
            .catch((err) => {
                console.error(`Error during postToConnection: ${err}`)
                return removeConnection(id)
            })
    }
}

export const handler = ws(
  ws.handler({
    // Connect
    async connect ({ id, event, context }) {
   //   console.log(['connect', id, event])

      const docName = getDocName(event)

      await addConnection(id, docName)

      // get doc from db
      // create new doc with no updates if no doc exists
      // const doc = await getOrCreateDoc(docName)
      //
      // // writeSyncStep1 (send sv)
      // const encoder = encoding.createEncoder()
      // encoding.writeVarUint(encoder, messageSync)
      // syncProtocol.writeSyncStep1(encoder, doc)

      // TODO cannot send message during connection!!!!!
      // await send({ context, message: encoding.toUint8Array(encoder), id })

      console.log('done connect')
      return { statusCode: 200, body: 'Connected.' }
    },

    // Disconnect
    async disconnect ({ id, event }) {
   //   console.log(['disconnect', id, event])

      await removeConnection(id)

      return { statusCode: 200, body: 'Disconnected.' }
    },

    // Message
    async default ({ message, id, event, context }) {
     // console.log(['message', id, message, event])

      message = fromBase64(message)

      const docName = (await getConnection(id)).DocName.S
      const connectionIds = await getConnectionIds(docName)
      const otherConnectionIds = connectionIds.filter(_ => _ !== id)
      const broadcast = (message) => {
        return Promise.all(otherConnectionIds.map(id => {
          return send({ context, message, id })
        }))
      }

      const encoder = encoding.createEncoder()
      const decoder = decoding.createDecoder(message)
      const messageType = decoding.readVarUint(decoder)

      switch (messageType) {
        // Case sync1: Read SyncStep1 message and reply with SyncStep2 (send doc to client wrt state vector input)
        // Case sync2 or yjsUpdate: Read and apply Structs and then DeleteStore to a y instance (append to db, send to all clients)
        case messageSync:
          encoding.writeVarUint(encoder, messageSync)

          // syncProtocol.readSyncMessage
          const messageType = decoding.readVarUint(decoder)
          let doc = null;
          switch (messageType) {
            case syncProtocol.messageYjsSyncStep1:
              doc = await getOrCreateDoc(docName)
              syncProtocol.writeSyncStep2(encoder, doc, decoding.readVarUint8Array(decoder))
              // Reply with our state
              if (encoding.length(encoder) > 1) {
                console.log("MICHAL: Reply with our state");

                await send({ context, message: encoding.toUint8Array(encoder), id })
              }
              break
            case syncProtocol.messageYjsSyncStep2:
            case syncProtocol.messageYjsUpdate:
              const update = decoding.readVarUint8Array(decoder)
              await updateDoc(docName, update)
              await broadcast(message)
              break
            default:
              throw new Error('Unknown message type')
          }

          break
        case messageAwareness: {
          await broadcast(message)
          break
        }
      }

      return { statusCode: 200, body: 'Data sent.' }
    },
  })
)
