#TODO: Offer pattern to set length
STRING_LEN=7

RAND_STRING=$(shuf -er -n"$STRING_LEN" {A..Z} {a..z} {0..9})
export RAND_STRING