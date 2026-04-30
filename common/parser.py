def parse(line):
    parts = line.split()

    if len(parts) < 4:
        return None

    try:
        return {
            "project": parts[0],
            "title": parts[1],
            "hits": int(parts[2]),
            "size": int(parts[3])
        }
    except:
        return None