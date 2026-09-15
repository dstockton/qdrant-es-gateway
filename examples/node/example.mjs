import { Client } from '@elastic/elasticsearch';

const client = new Client({ node: 'http://localhost:9200' });
await client.index({ index: 'docs', id: 'node-1', document: { title: 'Qdrant application search' } });
console.log(await client.search({ index: 'docs', query: { match: { title: 'application search' } } }));
