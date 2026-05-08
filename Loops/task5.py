from pyspark import AccumulatorParam
from common.parser import parse


class ProjectMaxHitsAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}

    def addInPlace(self, existing, new_partition):
        for project, (title, hits) in new_partition.items():
            if project not in existing or hits > existing[project][1]:
                existing[project] = (title, hits)
        return existing

class Task5Loops:
    def run(self, sc, path):
        rdd = sc.textFile(path)
        max_hits_acc = sc.accumulator({}, ProjectMaxHitsAccumulator())

        def process_partition(iterator):
            partition_best = {}
            for line in iterator:
                parsed = parse(line)
                if not parsed:
                    continue
                project = parsed["project"]
                # project = parsed["project"].split(".")[0]
                title = parsed["title"]
                hits = parsed["hits"]
                if project not in partition_best or hits > partition_best[project][1]:
                    partition_best[project] = (title, hits)
            max_hits_acc.add(partition_best)

        rdd.foreachPartition(process_partition)

        sorted_result = sorted(max_hits_acc.value.items(), key=lambda x: x[0])
        lines = []
        for project, (title, hits) in sorted_result:
            line = f"Project: {project}, Title: {title} --> Hits: {hits}"
            lines.append(line)
        output = "For each project the page title that received the highest number of hits (Using Loops):\n"
        output += "\n".join(lines)
        print(output)
        return output