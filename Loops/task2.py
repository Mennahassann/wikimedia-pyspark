from common.parser import parse

class Task2Loops:
    def run(self, path):
        total_pages_ends_with_img = 0
        not_en_project = 0
        
        IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".svg" ".webp")

        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                parsed = parse(line)
                if parsed:
                    if parsed["title"].lower().endswith(IMAGE_EXTENSIONS):
                        total_pages_ends_with_img +=1
                        if parsed["project"] != "en":
                            not_en_project += 1
            
            result = f"Total pages that end with image extensions: {total_pages_ends_with_img}\nPages that end with image extensions and are not in the 'en' project: {not_en_project}"
            
            print(result)
            return result