with open("history_1105630.csv", "r") as handle:
    cpi_lines = handle.readlines()[-365:]

    for line in cpi_lines:
        tokens = line.rstrip().split(",")
        if (tokens[2]) != "Installs":
            timestamp = tokens[0]
            spend = tokens[1]
            installs = int(tokens[2])
            cpi = tokens[3]
            org_id = "1105630"
            print("Adding cpi line:", timestamp, spend, installs, cpi, org_id)
            table.put_item(
                Item={
                    'timestamp': timestamp,
                    'spend': spend,
                    'installs': installs,
                    'cpi': cpi,
                    'org_id': org_id
                }
            )