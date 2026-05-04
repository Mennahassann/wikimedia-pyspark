from common.parser import parse

class Task4Loops:
    def run(self, path):
        project_hits = {}

        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                parsed = parse(line)
                if parsed:
                    project = parsed["project"]
                    hits = parsed["hits"]
                    
                    project_hits[project] = project_hits.get(project, 0) + hits

        # Sort by hits descending and take top 5
        sorted_projects = sorted(project_hits.items(), key=lambda x: x[1], reverse=True)
        top_5 = sorted_projects[:5]

        # Format results
        output = "Top 5 projects with highest total combined page hits (Using Loops):\n"
        for project, hits in top_5:
            output += f"Project: {project}, Total Hits: {hits}\n"

        print(output)
        return output
