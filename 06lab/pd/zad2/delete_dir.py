from snakebite.client import Client


cli = Client('localhost', 9000)

for dir in cli.delete(['/mojkatalog'], recurse=True):
	print(dir)
