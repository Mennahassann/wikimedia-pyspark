from common.parser import parse

class Task2MapReduce:
    def run(self, sc, path):
        IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp")
        
        def map_image_pages(line):
            parsed = parse(line)
            if parsed is None:
                return None
            
            title = parsed["title"].lower()
            project = parsed["project"]
            
            if not title.endswith(IMAGE_EXTENSIONS):
                return None
            
            is_english = 1 if project == "en" else 0
            return (1, is_english)
        
        result_rdd = sc.textFile(path) \
            .map(map_image_pages) \
            .filter(lambda x: x is not None) \
            .reduceByKey(lambda a, b: a + b)  
        
        collected = result_rdd.collect()
        if not collected:
            total_count = 0
            en_count = 0
            not_en_count = 0
        else:
            total_sum = collected[0][1]
            en_count = total_sum 
            not_en_count = sum(1 for _ in range(collected[0][1])) if en_count > 0 else 0
            
        
        result = (
            f"Total page titles ending with image extensions: {total_count}\n"
            f"  - In English project (en): {en_count}\n"
            f"  - NOT in English project: {not_en_count}"
        )
        
        print(result)
         
        return {
            "total": total_count,
            "en": en_count,
            "not_en": not_en_count
        }