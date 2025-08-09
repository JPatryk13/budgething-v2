from abc import ABC, abstractmethod

_err_msg = "This method should be overridden by subclasses."


class ReaderBase(ABC):

    @abstractmethod
    def read(self, *args, **kwargs):
        raise NotImplementedError(_err_msg)

    @abstractmethod
    def get_all(self, *args, **kwargs):
        raise NotImplementedError(_err_msg)


class WriterBase(ABC):

    @abstractmethod
    def create(self, *args, **kwargs):
        raise NotImplementedError(_err_msg)

    @abstractmethod
    def update(self, *args, **kwargs):
        raise NotImplementedError(_err_msg)

    @abstractmethod
    def delete(self, *args, **kwargs):
        raise NotImplementedError(_err_msg)
