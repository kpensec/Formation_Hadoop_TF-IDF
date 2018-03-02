package org.formation.hadoop;


class WordDocumentIDWritable implements Writable {
    private String word = "";
    private String documentID = "";

    void set(String word, String documentID) {
        setWord(word);
        setDocumentID(word);
    }

    void setWord(String word) {
        this.word = new String(word);
    }

    void setDocumentID(String documentID) {
        this.documentID = new String(documentID);
    }

    int compareTo(WordDocumentIDWritable lhs) {

        int result = this.documentID.compareTo(lhs.documentID);
        if (result != 0) {
            result = this.word.compareTo(lhs.word);
        }
        return result;
    }

    String hashCode() {
        return (word+documentID).hashCode();
    }

}

