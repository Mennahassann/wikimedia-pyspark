from common.parser import parse
class Task5Loops:
    def run(self, path):
        highest_project_hits = {}

        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                parsed = parse(line)
                if parsed:
                    project = parsed["project"]
                    title = parsed["title"]
                    hits = parsed["hits"]

                    current = highest_project_hits.get(project)
                    if current is None or hits > current[1]:
                        highest_project_hits[project] = (title, hits)

        sorted_result = sorted(highest_project_hits.items(), key=lambda x: x[0])
        lines = []
        for project, (title, hits) in sorted_result:
            line = f"Project: {project}, Title: {title} --> Hits: {hits}"
            lines.append(line)
        output = "For each project the page title that received the highest number of hits (Using Loops):\n"
        output += "\n".join(lines)        
        print(output)
        return output
