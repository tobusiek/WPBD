from snakebite.client import Client


cli = Client('localhost', 9000)

for dir in cli.mkdir(['/mojkatalog/input'], create_parent=True):
	print(dir)
