"""Bounded checks of known synthetic fixtures; never a general translation judge."""
import re
import unicodedata

SLOTS = (0,20,40,55,80,100,120)
FIXTURES = {
 'outgoing': ('en-GB','fr',(
  'The blue document is ready. We need two tickets for Friday.',
  'Do not cancel the meeting. It starts at nine, not ten.',
  'Delivery is on Tuesday. The price is twenty-seven euros.',
  'For the third order, please keep the yellow folder and send five copies of the signed document to the reception desk tomorrow morning.',
  'The train leaves at fourteen thirty. There are six passengers.',
  'Please confirm that the number is eighteen, not eighty.',
  'This is the last message. The appointment is on Thursday. Please keep the original.')),
 'incoming': ('fr-FR','en',(
  'Le dossier vert contient quatre pages. La réunion est lundi.',
  'Ne supprimez pas le document. Le rendez-vous est à quinze heures, pas seize heures.',
  'La livraison est mercredi. Le prix est de trente-neuf euros.',
  "Pour la quatrième commande, veuillez conserver le dossier rouge et envoyer sept copies du document signé au bureau d'accueil demain après-midi.",
  'Le train part à seize heures vingt. Il y a huit passagers.',
  'Veuillez confirmer que le nombre est treize, et non trente.',
  "C'est le dernier message. Le rendez-vous est vendredi. Veuillez garder l'original.")),
}

NUMBERS = {
 'quatre vingt':80,'quatre vingts':80,'dix huit':18,'vingt sept':27,
 'trente neuf':39,'twenty seven':27,'thirty nine':39,
 'quatorze':14,'quinze':15,'seize':16,'treize':13,'trente':30,
 'eighteen':18,'eighty':80,'thirteen':13,'thirty':30,'fourteen':14,'fifteen':15,
 'sixteen':16,'twenty':20,'neuf':9,'dix':10,'cinq':5,'deux':2,'quatre':4,
 'sept':7,'huit':8,'six':6,'two':2,'four':4,'five':5,'seven':7,'eight':8,
 'nine':9,'ten':10,
}
RULES = {
 'outgoing': {
  'blue_document':r'document.{0,30}bleu',
  'two_tickets':r'\b2\b.{0,20}(?:billets|tickets)',
  'friday':r'\bvendredi\b',
  'do_not_cancel':r'(?:n annulez pas|ne pas annuler|ne devez pas annuler|ne faut pas annuler).{0,30}reunion',
  'nine_not_ten':r'\b9\b.{0,35}(?:pas|non).{0,20}\b10\b',
  'tuesday':r'\bmardi\b',
  'price_27_euros':r'\b27\b.{0,12}euros?',
  'boundary_yellow_folder':r'(?:dossier|chemise).{0,20}jaune',
  'boundary_five_copies':r'\b5\b.{0,20}(?:copies|exemplaires)',
  'boundary_reception':r'\b(?:accueil|reception)\b',
  'boundary_tomorrow_morning':r'demain matin',
  'train_14_30':r'\b14\b.{0,15}\b30\b',
  'six_passengers':r'\b6\b.{0,20}(?:passagers|voyageurs)',
  '18_not_80':r'\b18\b.{0,35}(?:pas|non).{0,20}\b80\b',
  'thursday':r'\bjeudi\b',
  'keep_original':r'(?:gard|conserv)\w*.{0,30}original',
 },
 'incoming': {
  'green_folder':r'green.{0,20}(?:folder|file)',
  'four_pages':r'\b4\b.{0,10}pages',
  'monday':r'\bmonday\b',
  'do_not_delete':r'(?:do not|don t|must not|should not).{0,30}(?:delete|remove)',
  '15_not_16':r'\b15\b.{0,35}(?:not|rather than).{0,20}\b16\b',
  'wednesday':r'\bwednesday\b',
  'price_39_euros':r'\b39\b.{0,12}euros?',
  'boundary_red_folder':r'red.{0,20}(?:folder|file)',
  'boundary_seven_copies':r'\b7\b.{0,20}copies',
  'boundary_reception':r'\b(?:reception|welcome|front desk)\b',
  'boundary_tomorrow_afternoon':r'tomorrow afternoon',
  'train_16_20':r'\b16\b.{0,15}\b20\b',
  'eight_passengers':r'\b8\b.{0,20}(?:passengers|travelers|travellers)',
  '13_not_30':r'\b13\b.{0,35}(?:not|rather than).{0,20}\b30\b',
  'friday':r'\bfriday\b',
  'keep_original':r'(?:keep|retain|preserve).{0,25}original',
 },
}


def normalize(text):
    text=unicodedata.normalize('NFKD',text.lower())
    text=''.join(c for c in text if not unicodedata.combining(c))
    text=re.sub(r'[^a-z0-9]+',' ',text)
    for word,n in sorted(NUMBERS.items(),key=lambda x:-len(x[0])):
        text=re.sub(r'\b'+word+r'\b',str(n),text)
    return re.sub(r'\s+',' ',text).strip()


def check_facts(direction,text):
    text=normalize(text)
    # A miss is flagged for review: legitimate paraphrases may escape these patterns.
    counts={name:len(re.findall(pattern,text)) for name,pattern in RULES[direction].items()}
    return {'fact_matches':counts,'matched':sum(v>0 for v in counts.values()),
            'expected':len(counts),'flagged_for_review':[k for k,v in counts.items() if v==0],
            'repeated_transcript_facts':[k for k,v in counts.items() if v>1],
            'general_translation_accuracy_verified':False}
