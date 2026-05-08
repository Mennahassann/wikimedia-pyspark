from pyspark import AccumulatorParam
from common.parser import parse


class ProjectHitsAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}

    def addInPlace(self, acc1, acc2):
        for project, hits in acc2.items():
            acc1[project] = acc1.get(project, 0) + hits
        return acc1


class Task4Loops:
    def run(self, sc, path):
        project_hits_acc = sc.accumulator({}, ProjectHitsAccumulator())

        def process_partition(iterator):
            partition_projects = {}
            for line in iterator:
                parsed = parse(line)
                if not parsed:
                    continue
                project = parsed["project"]
                hits = parsed["hits"]
                partition_projects[project] = partition_projects.get(project, 0) + hits

            project_hits_acc.add(partition_projects)

        sc.textFile(path).foreachPartition(process_partition)

        # Sort by hits descending and take top 5
        sorted_projects = sorted(
            project_hits_acc.value.items(),
            key=lambda x: x[1],
            reverse=True
        )
        top_5 = sorted_projects[:5]

        # Format results
        output = "Top 5 projects with highest total combined page hits (Using Loops):\n"
        for project, hits in top_5:
            output += f"Project: {project}, Total Hits: {hits}\n"

        print(output)
        return output
