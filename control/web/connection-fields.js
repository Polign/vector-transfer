function node(tag, text, cls) { const n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (cls) n.className = cls; return n; }
const providerNames = { polign: 'Polign', pinecone: 'Pinecone', weaviate: 'Weaviate', milvus: 'Milvus', elasticsearch: 'Elasticsearch', opensearch: 'OpenSearch', pgvector: 'pgvector', s3vectors: 'S3 Vectors', chroma: 'Chroma', redis: 'Redis', mongodb: 'MongoDB', faiss: 'FAISS', solr: 'Solr', qdrant: 'Qdrant', turbopuffer: 'Turbopuffer' };
const providerFields = {
  weaviate: [['collection', 'Collection'], ['tenant', 'Tenant (optional)', '', false]],
  milvus: [['database', 'Database', 'default'], ['collection', 'Collection'], ['id_field', 'INT64 primary key field', 'id'], ['vector_field', 'Dense vector field', 'vector']],
  elasticsearch: [['index', 'Index'], ['id_field', 'Unique sortable ID field (must equal _id)', 'id'], ['vector_field', 'Dense vector field', 'vector']],
  opensearch: [['index', 'Index'], ['id_field', 'Unique sortable ID field (must equal _id)', 'id'], ['vector_field', 'Vector field', 'vector']],
  pgvector: [['database', 'Database'], ['collection', 'Table in public schema'], ['id_field', 'Primary key column', 'id'], ['vector_field', 'Vector column', 'vector'], ['metadata_field', 'JSONB metadata column', 'metadata']],
  chroma: [['tenant', 'Tenant', 'default_tenant'], ['database', 'Database', 'default_database'], ['collection', 'Collection UUID']],
  redis: [['database', 'Database number', '0'], ['key_prefix', 'HASH key prefix', 'vectors:'], ['vector_field', 'FLOAT32 vector field', 'vector'], ['metadata_field', 'JSON metadata field', 'metadata']],
  mongodb: [['database', 'Database'], ['auth_database', 'Authentication database', 'admin'], ['collection', 'Collection'], ['vector_field', 'Dense vector field', 'vector']],
  faiss: [['collection', 'Collection configured in FAISS adapter']],
  solr: [['collection', 'Collection / core'], ['id_field', 'Unique key field', 'id'], ['vector_field', 'Stored dense vector field', 'vector']],
  qdrant: [['collection', 'Collection']]
};
const providerHints = {
  weaviate: 'Uses unnamed dense vectors and UUID record IDs.',
  milvus: 'Uses INT64 primary keys with AutoID disabled. Keep the source unchanged during transfers and resumes.',
  elasticsearch: 'The ID field must be unique, sortable with doc values, and match each document’s _id. Elasticsearch 8.19+.',
  opensearch: 'Use a unique sortable ID field matching _id. Configure basic authentication below.',
  pgvector: 'Use an existing table with a unique primary key, vector column, and JSONB metadata column. TLS certificate verification is required.',
  chroma: 'Uses Chroma API v2 and a collection UUID. Destination credentials need read and write access to replace metadata.',
  redis: 'Uses standalone Redis HASH records with FLOAT32 little-endian vectors. Destination writes replace the HASH, including its TTL.',
  mongodb: 'All source _id values must use the selected type. BSON metadata travels as canonical Extended JSON. TLS is required.',
  faiss: 'Enter the HTTPS origin of the included FAISS adapter service. The adapter reads and writes index files on its own server.',
  solr: 'Vector and metadata fields must be stored. The selected ID field must be the schema’s uniqueKey.',
  qdrant: 'Uses unnamed dense vectors and UUID or unsigned integer record IDs.'
};
function credentialKeys(kind) { return kind === 's3vectors' ? ['access_key_id', 'secret_access_key', 'session_token'] : ['pgvector', 'redis', 'mongodb', 'opensearch', 'solr'].includes(kind) ? ['username', 'password'] : ['api_key']; }
function addInput(container, name, title, {type = 'text', required = true, placeholder = '', value = ''} = {}) {
  const label = node('label', title), input = node('input'); input.name = name; input.type = type; input.required = required; input.placeholder = placeholder; input.value = value;
  input.maxLength = type === 'password' ? 16384 : 2048; input.autocomplete = type === 'password' ? 'new-password' : 'off';
  label.append(input); container.append(label); return input;
}
function addSelect(container, name, title, options) {
  const label = node('label', title), select = node('select'); select.name = name;
  for (const [value, text] of options) { const option = node('option', text); option.value = value; select.append(option); }
  label.append(select); container.append(label); return select;
}
function credentialFields(container, prefix, kind) {
  if (kind === 's3vectors') {
    addInput(container, prefix + 'access_key_id', 'AWS access key ID', {placeholder: 'Access key for this database'});
    addInput(container, prefix + 'secret_access_key', 'AWS secret access key', {type: 'password'});
    addInput(container, prefix + 'session_token', 'AWS session token (if using temporary credentials)', {type: 'password', required: false});
  } else if (credentialKeys(kind).includes('password')) {
    addInput(container, prefix + 'username', kind === 'redis' ? 'ACL username (optional)' : 'Username', {required: kind !== 'redis'});
    addInput(container, prefix + 'password', 'Password', {type: 'password'});
  } else addInput(container, prefix + 'api_key', kind === 'milvus' ? 'API token (or username:password)' : 'API key', {type: 'password'});
}
