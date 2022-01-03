# web-search-engine
I have developed a web-search engine that using Node.js:
1. Creates inverted indices on the common-crawl's latest crawl-set,
2. Evaluates conjunctive and disjunctive queries using BM25 ranking,
3. Provides user with most relevant results, with a snippet.

<b>I/O efficiency: </b>
1. Tokenizer: Tokenizer reads the compressed WET-files using a read- stream which is piped into an uncompressor-stream that outputs a document as soon as it is parsed. Hence we never load the entire WET file in memory or explictly uncompress the WET files. All the PageTable entries and unsorted-posting entries are written to their re- spective write-streams which regularly flush to the disk hence not hold- ing too much data in memory.

2. Sorter: ’bsort’ helps with sorting binary files in-place. For bookmarking the sorted temp files were read in a buffered fashion with a buffer of size 5MB hense not rasing the memory profile of the program.

3. Merger: Merger holds only the termId headers from bookmarked files in the memory at any given time. These headers contain docIdBlock and frequencyBlock information in them, when all the headers concerning a particular termId are collected the byte blocks of length specified in header are read from the bookmarked files using Buffered reads and dumped into the final output using write-streams, hence achieving IO efficiency.

4. Query-Processor: Query processor refrains from reading the index file into the memory. It does random-seeks into memory based on the byte- offset information that it reads from the lexicon. The processor also decomprsses the block only when required and not in advance hence achieving the IO efficiency.

<b>Results from the largest crawl perfromed</b>:
1. The demo was based on 8668815(8 million) documents that were in- dexed.

2. Lexicon contained 8344880 distinct terms. 

3. The index was built in 20 batches, where each batch contained 10 WET files.

4. The unsorted postings per batch were of size aprrox. 800MB in binary format while the bookmarked-and-soreted postings were approx. 450MB.

5. The final index with var-byte-gap-compressed docId blocks and va- byte-compressed block was 3.77GB.

6. Lexicon with byte offset was of size 301.4MB

7. PageTable with an entry (documentId, url and document length) per document was of size 796.3MB

8. Tokenizing an entire WET files took less than a minute, bsort helped sorting the binary temp files per batch in under a minute and an extra minute was required   to bookmark the sorted files. The final merging took more than 30 minutes to complete.

9. Conjuctive quries returned results with 1-2 seconds while disjuctive queries returned results within 4-5 seconds.

<b>How to compile and run the code:</b>

Go to the root of the project directory and type following commands:

>> npm install (installs all the dependencies of the project)

>> npm run build (builds a production-build)

>> npm run prod (runs the production-build)
