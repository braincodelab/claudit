from abc import ABCMeta, abstractmethod

class IDummyFactory(metaclass=ABCMeta):

    "dummy factory interface"
    
    @staticmethod
    @abstractmethod
    def create_object():
        "abstract interface method"
        
