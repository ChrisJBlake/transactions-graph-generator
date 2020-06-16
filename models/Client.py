from .Node import Node

class Client(Node):
	def __init__(self, _id: int, offset: int):
		self.__type = 'Client'
		self.id = _id + offset
