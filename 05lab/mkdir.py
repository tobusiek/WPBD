from snakebite.client import Client


cli = Client('localhost', 9000)

for dir in cli.mkdir(['/books/detective_stories'], create_parent=True):
	print(dir)
