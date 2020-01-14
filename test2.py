# embedding lookup table, 1 lrweight, 2 display, 3 ctr
# l1 model -> modelckpt ->-> lrweight dict
# l2 model -> lrweight, display, ctr
# model refresh:
# 1. search old field index lrweight, display, ctr from voca
# load: dict = json or pickle .loads()
# lrweight value =
# 2. train l2 model, get update auc and copc
# 3. train l1 model
# 4. update lrweight, display, ctr, get join auc and copc


class L2:
    def inference(self, idfea):
        # lr w, b init
        self._initialize_weights()
        vocab = self.load_vocab(weight_file)
        # summary? lookup sparse
        tensor_array = []

        fields = ["userName", "sex", "chaterId"]
        for f in fields:
            # numpy vectors
            # normalization already done when generate weight file
            # wdc shape:[field alloc size * 3]
            wdc = vocab[f]["weights"]
            # variable trainable = false
            w = tf.Variable(wdc, name="wdc", trainable=False)
            wdc_tensor = tf.nn.embedding_lookup_sparse(w, idfea, None, combiner=sum)
            tensor_array.append(wdc_tensor)

        # concat_tensor shape:[batchsize * (3*fieldcount) ]
        concat_tensor = tf.concat(tensor_array)
        w = self.weights["w"]
        b = self.weights["b"]
        logit = concat_tensor * w + b

class L1:
    def inference(self, id):
        # lr only have y = sigmoid(w*x)
        # store all weight into w1.
        # copy w1 to w2 if display > 5
        # use w2 serve L2 model