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

            # (total pages, english pages)
            is_english = 1 if project == "en" else 0
            return (1, is_english)

        result = (
            sc.textFile(path)
            .map(map_image_pages)
            .filter(lambda x: x is not None)
            .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        )

        total_count = result[0]
        en_count = result[1]
        not_en_count = total_count - en_count

        output = (
            f"Total page titles ending with image extensions: {total_count}\n"
            f"In English project (en): {en_count}\n"
            f"NOT in English project: {not_en_count}"
        )

        print(output)
        return output