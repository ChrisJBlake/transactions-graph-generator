from .Node import Node

class Company(Node):
	def __init__(self, _id: int, offset: int):
		self.__type = 'Company'
		self.id = _id + offset  # To match my weird schema where the first org id was the number of people
