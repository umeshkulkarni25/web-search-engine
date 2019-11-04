import mongoose from 'mongoose';

const documentSchema = new mongoose.Schema({
  docId: {
    type: Number,
    required: true,
  },
  content: {
    type: String,
    required: true,
  },
});

documentSchema.index({ docId: 1 });
export default mongoose.model('Document', documentSchema);
