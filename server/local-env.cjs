process.env.REGION = 'eu-central-1' // same as aws cli config
process.env.DYNAMODB_ENDPOINT = 'http://localhost:8000'
process.env.DOCS_TABLE_NAME = 'docs'
process.env.CONNECTIONS_TABLE_NAME = 'connections'
process.env.PORT = 9000
process.env.MESSAGE_MAX_CHUNK_SIZE_IN_BYTES = String(100 * 1024)
