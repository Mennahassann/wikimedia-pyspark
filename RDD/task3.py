import re
import time

class RDDTask3:
    def extract_words(self, line):
            try:
                parts = line.split(" ")
                if len(parts) >= 2:
                    title = parts[1]
                    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', title.lower())
                    words = [w for w in cleaned.split("_") if w]
                    return words
                return []
            except:
                return []
            
    def run(self, sc, path):
        start_time = time.time()

        rdd = sc.textFile(path)

        words_rdd = rdd.flatMap(self.extract_words)

        word_pairs = words_rdd.map(lambda word: (word, 1))

        word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
        
        top10 = word_counts.takeOrdered(10, key=lambda x: -x[1])

        print("Top 10 most frequent terms:")
        for i, (word, count) in enumerate(top10, 1):
            print(f"{i:2}. {word:<20} {count:>10,}")
        
        return time.time() - start_time