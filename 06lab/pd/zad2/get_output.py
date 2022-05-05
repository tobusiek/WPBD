from snakebite.client import Client


cli = Client('localhost', 9000)

for f in cli.copyToLocal(['/mojkatalog/output'], '/home/s26549/sharedfolder/WPBD/06lab/pd'):
	print(f)
