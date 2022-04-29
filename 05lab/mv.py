from snakebite.client import Client


cli = Client('localhost',9000)

for f in cli.rename(['/books/detective_stories/opowiesc.txt'], '/books_read'):
	print(f)
