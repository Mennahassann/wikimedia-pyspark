from common.parser import parse

class Task4RDD:
    def run(self, sc, path):
        data = sc.textFile(path)

        # Parse data and filter out None values
        parsed = data.map(parse).filter(lambda x: x is not None)

        # Map to (project_code, hits) and sum hits per project
        project_hits = (
            parsed.map(lambda x: (x["project"], x["hits"]))
            .reduceByKey(lambda a, b: a + b)
        )

        # Get top 5 projects by hits
        top_5 = project_hits.takeOrdered(5, key=lambda x: -x[1])

        # Format results
        output = "Top 5 projects with highest total combined page hits (Using RDD):\n"
        for project, hits in top_5:
            output += f"Project: {project}, Total Hits: {hits}\n"

        print(output)
        return output
