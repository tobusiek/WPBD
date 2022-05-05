from snakebite.client import Client


cli = Client('localhost', 9000)

for f in cli.copyToLocal(['/mojkatalog/output/part-00000'], '/home/s26549/WPBD/06lab/pd/output'):
	print(f)
