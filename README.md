# Toy implementation of Loki in Python

This code is the result of me wondering how ChatGPT / Copilot can help me make a simple program quickly. I also wanted to understand how columnary datastores work (Apache Arrow/Parquet)

The current state of the program can receive snappy encoded protocol buffer loglines from promtail, and it can respond to a simple instant log query from Grafana or LogCLI. 

