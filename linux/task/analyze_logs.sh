#!/bin/bash

> report.txt

echo "--- LOGS REPORT ---" >> report.txt
echo -n "Total requests: " >> report.txt
awk 'END {print NR}' access.log >> report.txt

echo "" >> report.txt

echo "Number of unique IP-addresses: " >> report.txt
awk '{ips[$1]++} END {print length(ips)}' access.log >> report.txt

echo "" >> report.txt

echo "Number of requests by methods: " >> report.txt
awk '{
  method=substr($6, 2);
  methods[method]++
}
END {
  for (m in methods)
	  print m ": " methods[m]
}' access.log >> report.txt

echo "" >> report.txt

echo "The most popular URL: " >> report.txt
awk '{
	urls[$7]++
}
END {
	max_count=0;
	popular_url="";
	for (u in urls) {
		if (urls[u] > max_count) {
			max_count = urls[u];
			popular_url = u;
		}
	}
	print popular_url " (requests: " max_count ")"
}' access.log >> report.txt

echo "Finished analyze. Results save to report.txt"
