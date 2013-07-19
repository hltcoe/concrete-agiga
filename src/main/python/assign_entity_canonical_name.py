'''
Create a canonical name string for each entity if it doesn't already exist.
To create a canonical name, examine each mention for an entity, extract the named
entity heads, and select one using a heuristic (longest, first, etc.)
We will set the entity type if its not present.
'''

import sys
from concrete.utils import parseCommandLine, usage
from concrete.proto_io import ProtocolBufferFileReader, ProtocolBufferFileWriter
from concrete_pb2 import Communication, Entity

class AssignEntityCanonicalName:
	def __init__(self):
		pass

	def processFile(self, input, output):
		reader = ProtocolBufferFileReader(Communication, filename=input)
		writer = ProtocolBufferFileWriter(filename=output)
		num_docs = 0
		num_entities = 0
		num_entities_set = 0
		for msg in reader:
			new_num_entities, new_num_entities_set = self.processCommunication(msg)
			num_entities_set += new_num_entities_set
			num_entities += new_num_entities
			
			num_docs += 1
			
			writer.write(msg)
			if num_docs % 100 == 0:
				sys.stdout.write(str(num_docs) + '\r')
				sys.stdout.flush()
			
		reader.close()
		writer.close()
		print 'Processed %d communications.' % num_docs
		print 'Processed %d entities.' % num_entities
		print 'Assigned %d canonical names.' % num_entities_set
		
	def processCommunication(self, communication):
		# Start by finding the spans of the named entities in the communication
		# and their token offsets.
		num_entities = 0
		num_entities_set = 0
		tokenization_uuid_to_named_entities = {}
		
		text = communication.text
		section_segmentation = communication.section_segmentation[0]
		for section in section_segmentation.section:
			section_start = section.text_span.start
			section_end = section.text_span.end
			
			sentence_segmentation = section.sentence_segmentation[0]
			for sentence in sentence_segmentation.sentence:
				sentence_start = sentence.text_span.start
				sentence_end = sentence.text_span.end
				
				tokenization = sentence.tokenization[0]
				
				#tokenization.token[ii].text_span.start/end
				#tokenization.token[ii].token_index
				
				token_tagging = tokenization.ner_tags[0]
				tokenization_uuid = self.getUUIDString(tokenization.uuid)
				# Extract a list of the named entities in this tokenization
				last_tag = None
				current_ne = []
				token_indexes = []
				current_type = None

				for tagged_token in token_tagging.tagged_token:
					tag = tagged_token.tag
					token_index = tagged_token.token_index
					word = tokenization.token[token_index].text
					if last_tag != tag:
						last_tag = tag
						if len(current_ne) > 0:
							if current_ne != None:
								tokenization_uuid_to_named_entities.setdefault(tokenization_uuid, []) \
									.append((self.constructNameEntityString(tokenization.token, token_indexes), current_type, token_indexes))
							current_ne = []
							token_indexes = []
							current_type = None
					if tag == 'LOCATION' or tag == 'ORGANIZATION' or tag == 'PERSON':
						current_ne.append(word)
						current_type = tag
						token_indexes.append(token_index)
						
		# We now have a list of all the entities in the communication.
		# Walk the entity_mention_set and get for each mention, the NER head string.
		# Store these in a mapping between mention UUID and head string.
		uuid_to_head_mention = {}
		
		for entity_mention in communication.entity_mention_set[0].mention:
			entity_mention_uuid = self.getUUIDString(entity_mention.uuid)
			anchor_token_index = entity_mention.tokens.anchor_token_index
			tokenization_uuid = self.getUUIDString(entity_mention.tokens.tokenization_id)
			# Get all the named entities that were in this tokenization_uuid
			if tokenization_uuid in tokenization_uuid_to_named_entities:
				for entry in tokenization_uuid_to_named_entities[tokenization_uuid]:
					# Do any of these named entities overlap the anchor_token_index
					token_indexes = set(entry[2])
					if anchor_token_index in token_indexes:
						# This named entity overlaps with the mention head.
						# We will say that this named entity is the full mention head
						# for this entity reference.
						uuid_to_head_mention[entity_mention_uuid] = entry
						break
		# We now have a list of all the named entities that are heads for the entity mentions.
		# Come up with a canonical name for every entity.
		
		for entity in communication.entity_set[0].entity:
			num_entities += 1

			if entity.canonical_name != None and len(entity.canonical_name) > 0:
				continue
			# Otherwise, let's come up with a canonical name.
			candidates = []
			for mention_id in entity.mention_id:
				mention_uuid = self.getUUIDString(mention_id)
				# Do we know of any named entities for this mention?
				if mention_uuid in uuid_to_head_mention:
					entry = uuid_to_head_mention[mention_uuid]
					candidates.append(entry)
			

			# Let's consider the candidates.
			# Check that the entity type and canonical name are compatible
			filtered_candidates = []
			
			for candidate in candidates:
				if (entity.entity_type == Entity.PERSON and candidate[1] == 'PERSON') or \
					(entity.entity_type == Entity.ORGANIZATION and candidate[1] == 'ORGANIZATION') or \
					(entity.entity_type == Entity.GPE and candidate[1] == 'LOCATION') or \
					not entity.HasField('entity_type'): # The field is not set yet.
					filtered_candidates.append(candidate)
			# All the filtered_candidates are appropriate choices.
			
			# Heuristic time. Pick the first one.
			if len(filtered_candidates) > 0:
				canonical_name = filtered_candidates[0][0]
				entity.canonical_name = canonical_name
				num_entities_set += 1
				
				
				if not entity.HasField('entity_type'): # Set it based on the selected type.
					type = filtered_candidates[0][1]
					if type == 'PERSON':
						entity.entity_type = Entity.PERSON
					elif type == 'ORGANIZATION':
						entity.entity_type = Entity.ORGANIZATION
					elif type == 'LOCATION':
						entity.entity_type = Entity.GPE
					
		return num_entities, num_entities_set
	
	def constructNameEntityString(self, tokens, token_indexes):
		ne_string = []
		last_end = None
		for token_index in token_indexes:
			token = tokens[token_index]
			if last_end != None:
				num_spaces = token.text_span.start - last_end
				for ii in range(num_spaces):
					ne_string.append(' ')

			ne_string.append(token.text)
			last_end = token.text_span.end

		return ''.join(ne_string)
			
	def getUUIDString(self, uuid):
		return str(uuid.high) + '#' + str(uuid.low)
		
	def main(self):
		# Specify options.
		options = [ 
				['input=', 'The input file containing Communication protos.', True],
				['output=', 'Where to save the processed data.', True],
				]
				# Start main method here.
		
		command_line = '%s --input --output'
		options_hash, remainder = parseCommandLine(options, command_line=command_line)

		if (len(remainder) != 0):
			print usage(sys.argv, command_line, options)
			sys.exit()
		
		self.processFile(options_hash['input'], options_hash['output'])
			
		

if __name__ == '__main__':
	AssignEntityCanonicalName().main()