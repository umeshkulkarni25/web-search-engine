import mongoose from 'mongoose';

const { MONGODB_URL } = process.env;
if (!MONGODB_URL) {
  throw new Error('database url not provided');
}
mongoose.set('useCreateIndex', true); // added to get fix deprication warning, DeprecationWarning: collection.ensureIndex is deprecated. Use createIndexes instead.
const db = mongoose.connect(MONGODB_URL, { useNewUrlParser: true, useUnifiedTopology: true });
mongoose.connection.on('error', () => {
  throw new Error(`unable to connect to database: ${MONGODB_URL}`);
});
export default db;
