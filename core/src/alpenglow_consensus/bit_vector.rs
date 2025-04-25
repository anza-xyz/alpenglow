use thiserror::Error;

/// Maximum number of validators
///
/// There are around 1300 validators currently. For a clean power-of-two
/// implementation, we should chosoe either 2048 or 4096. Choose a more
/// conservative number 4096 for now.
const MAXIMUM_VALIDATORS: usize = 4096;

/// The number of bytes in a bit-vector to represent up to 4096 validators
/// (`MAXIMUM_VALIDATORS` / 8)
const VALIDATOR_BIT_MAP_U8_SIZE: usize = 512;

#[derive(Debug, Error, PartialEq)]
pub enum BitVectorError {
    #[error("Index out of bounds")]
    IndexOutOfBounds,
}

/// Validator bit map
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BitVector {
    pub data: [u8; VALIDATOR_BIT_MAP_U8_SIZE], // 2048 bits = 512 * 8 bits
}

impl Default for BitVector {
    fn default() -> Self {
        Self {
            data: [0u8; VALIDATOR_BIT_MAP_U8_SIZE],
        }
    }
}

impl BitVector {
    pub fn set_bit(&mut self, index: usize, value: bool) -> Result<(), BitVectorError> {
        if index >= MAXIMUM_VALIDATORS {
            return Err(BitVectorError::IndexOutOfBounds);
        }
        let (word_index, bit_index) = Self::word_index(index);
        if value {
            self.data[word_index] |= 1 << bit_index; // Set the bit
        } else {
            self.data[word_index] &= !(1 << bit_index); // Clear the bit
        }
        Ok(())
    }

    pub fn get_bit(&self, index: usize) -> Result<bool, BitVectorError> {
        if index >= MAXIMUM_VALIDATORS {
            return Err(BitVectorError::IndexOutOfBounds);
        }
        let (word_index, bit_index) = Self::word_index(index);
        Ok((self.data[word_index] & (1 << bit_index)) != 0)
    }

    /// Find the word index and bit index for a given index
    fn word_index(index: usize) -> (usize, usize) {
        // divide by 8 using bit shifts (2^3 = 8)
        let word_index = index >> 3;
        // find the remainder when divided by 8 using bit shifts (7 = 0b111)
        let bit_index = index & 7;
        (word_index, bit_index)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bit_vector() {
        let mut bit_vector = BitVector::default();

        bit_vector.set_bit(0, true).unwrap();
        bit_vector.set_bit(1, true).unwrap();
        bit_vector.set_bit(4094, true).unwrap();
        bit_vector.set_bit(4095, true).unwrap();

        assert!(bit_vector.get_bit(0).unwrap());
        assert!(bit_vector.get_bit(1).unwrap());
        assert!(!bit_vector.get_bit(2).unwrap());
        assert!(!bit_vector.get_bit(4093).unwrap());
        assert!(bit_vector.get_bit(4094).unwrap());
        assert!(bit_vector.get_bit(4095).unwrap());

        assert_eq!(
            bit_vector.set_bit(4096, true).unwrap_err(),
            BitVectorError::IndexOutOfBounds
        );

        assert_eq!(
            bit_vector.get_bit(4096).unwrap_err(),
            BitVectorError::IndexOutOfBounds
        );
    }
}
