"""
	Library for efficient binary input/output of non-nested data
	
	API:
	
		with Binary_IO(name=file_name, mode=file_mode) as bio:
		with Binary_IO(file_object=fo) as bio:
		with Binary_IO(io_object=iob) as bio:
			bio.write(something, type_descr)
			something = bio.read(type_descr)
			
		type_descr:
			== scalar types ==
			'b', 'B', 'i', 'I, 'l', 'L': numeric types as in struct.pack
			'str[/enc]', 'sstr[/enc]': string, short string (default enc is 'utf-8')
			'byt', 'sbyt': bytes, short bytes sequence
			== flat container types ==			
			'vec:element_type' vector of elements of scalar type 'element_type'
			'set:member_type' set of members of scalar type 'member_type'
			'map:key_type:value_type' map of key, value of scalar types 'key_type', 'value_type'
			
	Run module to test it
"""

import io
import struct
from contextlib import ContextDecorator

class ContextMixin(ContextDecorator):
	"""Mixin for the 'with' context handler"""

	def __enter__(self):
		if self.file_object is None:
			if self.io_object is None:
				self.file_object = self.open(self.name, '{}b'.format(self.mode))
			else:
				self.file_object = self.io_object
		return self

	def __exit__(self, *exc):
		self.file_object.close()
		return False

# static initialization of class variables

def static_initialization(klass):
	klass.NUMERIC_FMT = set(( 'b', 'B', 'i', 'I', 'l', 'L' ))
	klass.STRING_TYPES = set(( 'str', 'sstr' ))
	klass.BYTES_TYPES = set(( 'byt', 'sbyt' ))
	klass.SCALAR_TYPES = klass.NUMERIC_FMT | klass.STRING_TYPES | klass.BYTES_TYPES
	klass.DEFAULT_ENC = 'utf-8'
	klass.NUMBER_SIZES = { }
	klass.LONG_TYPE = 'I', None
	klass.SHORT_TYPE = 'B', None

	for fmt in klass.NUMERIC_FMT:
		klass.NUMBER_SIZES[fmt] = len(struct.pack(fmt, 42))

class BinaryIO(ContextMixin):
	def __init__(self, name=None, mode=None, file_object=None, io_object=None):
		self.name = name
		self.mode = mode
		self.file_object = file_object
		self.io_object = io_object

	def _in(self, length):
		return self.file_object.read(length)

	def _out(self, data):
		return self.file_object.write(data)

	def read_number(self, number_type):
		fmt, enc = number_type # enc is ignored for numbers
		if not fmt in self.NUMERIC_FMT:
			raise ValueError('format {} unsupported, number expected'.format(fmt))
		return struct.unpack(fmt, self._in(self.NUMBER_SIZES[fmt]))[0]
			
	def write_number(self, number, number_type):
		fmt, enc = number_type # enc is ignored for numbers
		return self._out(struct.pack(fmt, number))

	def read_byte(self, number):
		number_type = self.SHORT_TYPE
		return self.read_number(number_type)

	def write_byte(self, number):
		number_type = self.SHORT_TYPE
		return self.write_number(number, number_type)

	def _fmt_to_len_type(self, fmt):
		len_type = self.SHORT_TYPE
		if fmt == 'byt' or fmt == 'str':
			len_type = self.LONG_TYPE
		return len_type

	def _get_len_and_fmt(self, sequence, fmt):
		length = len(sequence)
		typ, enc = self._fmt_to_len_type(fmt)
		if typ == 'B' and length > 255:
			raise ValueError('not a short string/bytes sequence')
		return length, (typ, enc)
		
	def read_bytes(self, item_type):
		fmt, enc = item_type # enc is ignored for bytes
		number_type = self._fmt_to_len_type(fmt)
		length = self.read_number(number_type)
		return self._in(length)

	def write_bytes(self, _bytes, item_type):
		fmt, enc = item_type # enc is ignored for bytes
		length, number_type = self._get_len_and_fmt(_bytes, fmt)
		n = self.write_number(length, number_type)
		return n + self._out(_bytes)

	def _decode_string(self, _bytes, enc):
		if not enc:
			enc = self.DEFAULT_ENC
		return _bytes.decode(enc)

	def _encode_string(self, string, enc):
		if not enc:
			enc = self.DEFAULT_ENC
		return bytes(string, encoding=enc)

	def read_string(self, item_type):
		fmt, enc = item_type
		number_type = self._fmt_to_len_type(fmt)
		length = self.read_number(number_type)
		_bytes = self._in(length)
		return self._decode_string(_bytes, enc)

	def write_string(self, string, item_type):
		fmt, enc = item_type
		_bytes = self._encode_string(string, enc)
		length, number_type = self._get_len_and_fmt(_bytes, fmt)
		n = self.write_number(length, number_type)		
		return n + self._out(_bytes)

	def read_vector(self, element_type):
		vector = [ ]
		l = self.read_number(self.LONG_TYPE)
		for i in range(l):
			vector.append(self.read_scalar(element_type))
		return vector

	def write_vector(self, vector, element_type):
		if vector is None:
			vector = [ ]
		n = self.write_number(len(vector), self.LONG_TYPE)
		for element in vector:
			n += self.write_scalar(element, element_type)
		return n

	def read_set(self, member_type):
		s = set()
		l = self.read_number(self.LONG_TYPE)
		for i in range(l):
			s.add(self.read_scalar(member_type))
		return s

	def write_set(self, s, member_type):
		n = self.write_number(len(s), self.LONG_TYPE)
		for member in sorted(s):
			n += self.write_scalar(member, member_type)
		return n

	def read_map(self, key_type, value_type):
		m = { }		
		l = self.read_number(self.LONG_TYPE)
		for i in range(l):
			key = self.read_scalar(key_type)
			value = self.read_scalar(value_type)
			if key in m:
				raise ValueError('duplicate key {}'.format(key))
			m[key] = value			
		return m

	def write_map(self, m, key_type, value_type):
		n = self.write_number(len(m), self.LONG_TYPE)
		for key, value in sorted(m.items()):
			n += self.write_scalar(key, key_type)
			n += self.write_scalar(value, value_type)
		return n

	def read_scalar(self, item_type):
		typ, enc = item_type # enc is ignored here
		if typ in self.NUMERIC_FMT:
			return self.read_number(item_type)
		elif typ in self.STRING_TYPES:
			return self.read_string(item_type)
		elif typ in self.BYTES_TYPES:
			return self.read_bytes(item_type)
		else:
			raise ValueError('type {} unsupported, scalar expected'.format(typ))

	def write_scalar(self, item, item_type):
		typ, enc = item_type # enc is ignored here
		if typ in self.NUMERIC_FMT:
			return self.write_number(item, item_type)
		elif typ in self.STRING_TYPES:
			return self.write_string(item, item_type)
		elif typ in self.BYTES_TYPES:
			return self.write_bytes(item, item_type)
		else:
			raise ValueError('value_type {} unsupported, scalar expected'.format(typ))
			
	def parse_type_descr(self, type_descr):
		type_components = type_descr.split(':')
		l = [ ]		
		for type_component in type_components:
			typ = type_component
			enc = None
			if '/' in type_component:
				i = typ.index('/')
				typ = type_component[:i]
				enc = type_component[i+1:]
				
			l.append(( typ, enc ))
		return l

	def read(self, type_descr):
		type_components = self.parse_type_descr(type_descr)
		typ, enc = type_components[0] # enc is ignored here
		if typ in self.SCALAR_TYPES:
			return self.read_scalar(type_components[0])
		elif typ == 'vec':
			return self.read_vector(type_components[1])
		elif typ == 'set':
			return self.read_set(type_components[1])
		elif typ == 'map':
			return self.read_map(type_components[1], type_components[2])
		else:
			raise ValueError('type {} unsupported'.format(typ))

	def write(self, item, type_descr):
		type_components = self.parse_type_descr(type_descr)
		typ, enc = type_components[0] # enc is ignored here
		if typ in self.SCALAR_TYPES:
			return self.write_scalar(item, type_components[0])
		elif typ == 'vec':
			return self.write_vector(item, type_components[1])
		elif typ == 'set':
			return self.write_set(item, type_components[1])
		elif typ == 'map':
			return self.write_map(item, type_components[1], type_components[2])
		else:
			raise ValueError('type {} unsupported'.format(typ))

static_initialization(BinaryIO)

if __name__ == '__main__':
	def assert_deep_equal(lhs, rhs):
		assert type(lhs) == type(rhs)
		try:
			assert len(lhs) == len(rhs)
		except TypeError:
			# we get here if lhs is scalar
			assert lhs == rhs
			# if the assertion passes, we are done			
			return

		# we now know the objects are of the same, non scalar type

		if isinstance(lhs, str) or isinstance(lhs, bytes):
			assert lhs == rhs
		elif isinstance(lhs, list) or isinstance(lhs, tuple):
			for item1, item2 in zip(lhs, rhs):
				assert_deep_equal(item1, item2)
		elif isinstance(lhs, set):
			for item1, item2 in zip(lhs, rhs):
				assert_deep_equal(item1, item2)
		elif isinstance(lhs, dict):
			for key, value in lhs.items():
				assert key in rhs
				assert_deep_equal(value, rhs[key])
		else:
			raise TypeError('something weird just occured (types were: {} {})'.format(type(lhs), type(rhs)))

	the_number = 4711
	the_string = 'a quick brown fox jümps over the läzy d0g'
	the_byte_vector = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -19 ]
	the_string_vector = [ 'egg', 'spam', 'bacon', 'ham', 'räksmörgås' ]
	the_long_set = set( ( 9223372036854775807, 4611686018427387903, 2305843009213693951 ) )
	the_str_long_map = { 'xyzzy': 42, 'bar':4711, 'swag':31412359 }
	the_int_number_map = { 42967295: 1, -1:2, 314212359:65535 }
	the_bytes_sequence = bytes(str(globals())[179], 'ascii')

	the_things = [
		[ the_string, 'sstr/utf-8' ],
		[ the_number, 'i' ],
		[ the_byte_vector, 'vec:b' ],
		[ the_string_vector, 'vec:sstr/latin-1' ],
		[ the_long_set, 'set:L' ],
		[ the_str_long_map, 'map:str/ascii:L' ],
		[ the_int_number_map, 'map:i:I' ],
		[ the_bytes_sequence, 'sbyt' ] ]

	io_object=io.BytesIO()

	n = 0

	with BinaryIO(io_object=io_object) as bio:
		for item, type_descr in the_things:
			n += bio.write(item, type_descr)
		bytes_written = bytes(io_object.getbuffer())

	assert n == len(bytes_written)

	print(bytes_written)

	io_object=io.BytesIO(bytes_written)

	with BinaryIO(io_object=io_object) as bio:
		for item, type_descr in the_things:
			roundtrip = bio.read(type_descr)
			assert_deep_equal(item, roundtrip)

	io_object=io.BytesIO()

