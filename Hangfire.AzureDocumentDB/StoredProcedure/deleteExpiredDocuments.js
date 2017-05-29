/**
 * Expiration manager to delete old expired documents
 * @param {number} type - The type of the document to delete
 * @returns {number} number of documents deleted 
 */
function deleteExpiredDocuments(type) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();
    var expiryDate = Date.now();

    var result = collection.filter(function (doc) {
        // ignore raw counter 
        if (type === 4 && doc.type === type && doc.counter_type === 2)
            return false;
        return doc.type === type && doc.expire_on <= expiryDate;
    }, function (err, documents) {
        if (err) throw err;

        if (documents.length > 0) {
            for (var i = 0; i < documents.length - 1; i++) {
                var self = documents[i]._self;
                collection.deleteDocument(self);
            }
        }

        response.setBody(documents.length);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}