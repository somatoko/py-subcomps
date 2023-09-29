

# Extend existing in-memory index with data obtained from the
# supplied content.
def consume_doc(doc_id, doc_content, index):
    for word in doc_content.split():
        if word in index:
            if doc_id in index[word]:
                index[word][doc_id] += 1
            else:
                index[word][doc_id] = 1
        else:
            index[word] = {doc_id: 1}
    return index
