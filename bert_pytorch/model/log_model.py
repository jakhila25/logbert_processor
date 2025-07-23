import torch
import torch.nn as nn
from .bert import BERT

class BERTLog(nn.Module):
    """
    BERT Log Model
    """

    def __init__(self, bert: BERT, vocab_size):
        """
        :param bert: BERT model which should be trained
        :param vocab_size: total vocab size for masked_lm
        """
        super().__init__()
        self.bert = bert
        self.mask_lm = MaskedLogModel(self.bert.hidden, vocab_size)
        self.time_lm = TimeLogModel(self.bert.hidden)
        # self.fnn_cls = LinearCLS(self.bert.hidden)
        # self.cls_lm = LogClassifier(self.bert.hidden)

    def forward(self, x, time_info):
        x = self.bert(x, time_info=time_info)  # [batch, seq_len, hidden]

        cls_output = x[:, 0]  # [CLS] token vector from BERT

        return {
            "logkey_output": self.mask_lm(x),    # [batch, seq_len, vocab_size]
            "time_output": self.time_lm(x),      # optional
            "cls_output": cls_output,            # [batch, hidden]
            "cls_fnn_output": None,              # unused for now
            "token_embeddings": x[0]             # [seq_len, hidden] for first batch element
        }


class MaskedLogModel(nn.Module):
    """
    Predicting original token from masked input sequence
    """

    def __init__(self, hidden, vocab_size):
        super().__init__()
        self.linear = nn.Linear(hidden, vocab_size)
        self.softmax = nn.LogSoftmax(dim=-1)

    def forward(self, x):
        return self.softmax(self.linear(x))


class TimeLogModel(nn.Module):
    def __init__(self, hidden, time_size=1):
        super().__init__()
        self.linear = nn.Linear(hidden, time_size)

    def forward(self, x):
        return self.linear(x)


class LogClassifier(nn.Module):
    def __init__(self, hidden):
        super().__init__()
        self.linear = nn.Linear(hidden, hidden)

    def forward(self, cls):
        return self.linear(cls)


class LinearCLS(nn.Module):
    def __init__(self, hidden):
        super().__init__()
        self.linear = nn.Linear(hidden, hidden)

    def forward(self, x):
        return self.linear(x)
