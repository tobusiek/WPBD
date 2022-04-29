from snakebite.client import Client


cli = Client('localhost',9000)

for dir in cli.mkdir(['/books_read']):
	print(dir)
