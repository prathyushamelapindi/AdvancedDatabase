//
//  bitmap.c
//  Assign#03
//
//  Created by raghuveer vemuri on 10/30/15.
//  Copyright © 2015 raghuveer vemuri. All rights reserved.
//

//#include <limits.h>
//#include <stdint.h>   /* for uint32_t */
//
//
//typedef uint8_t word_t;
//
////enum { BITS_PER_WORD = sizeof(word_t) * CHAR_BIT };
//
//#define WORD_OFFSET(b) ((b) / CHAR_BIT)
//#define BIT_OFFSET(b)  ((b) % CHAR_BIT)
//
//void set_bit(word_t *words, int n) {
//    words[WORD_OFFSET(n)] |= (1 << BIT_OFFSET(n));
//}
//
//void clear_bit(word_t *words, int n) {
//    words[WORD_OFFSET(n)] &= ~(1 << BIT_OFFSET(n));
//}
//
//int get_bit(word_t *words, int n) {
//    word_t bit = words[WORD_OFFSET(n)] & (1 << BIT_OFFSET(n));
//    return bit != 0;
//}