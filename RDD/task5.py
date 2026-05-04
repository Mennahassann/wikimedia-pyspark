from common.parser import parse
class Task5:
    def run(self, sc, path):
        def map_to_key_value(line):
            parsed = parse(line)
            if parsed:
                return (parsed["project"], (parsed["title"], parsed["hits"]))

        def get_max_hits(a, b):
            if a[1]>=b[1]:
                return a
            else:
                return b

        result = (
            sc.textFile(path)
            .map(map_to_key_value)
            .filter(lambda x: x is not None)
            .reduceByKey(get_max_hits)
            .collect()
        )

        sorted_result = sorted(result, key=lambda x: x[0])
        lines = []
        for project, (title, hits) in sorted_result:
            line = f"Project: {project}, Title: {title} --> Hits: {hits}"
            lines.append(line)
        output = "For each project the page title that received the highest number of hits (Using RDD):\n"
        output += "\n".join(lines)

        print(output)
        return output
