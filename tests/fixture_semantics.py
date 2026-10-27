"""Explicit fact coverage of authored controls; not a general semantic judge."""
import re
from handover_checks import normalize


def prepared(text):
    text=text.replace('€',' euros ')
    text=re.sub(r'(\d)\s*[hH]\s*(?=\d)',r'\1 heures ',text)
    text=re.sub(r'(\d)[hH]\b',r'\1 heures',text)
    text=re.sub(r'(?<=\d)(?=[A-Za-z])|(?<=[A-Za-z])(?=\d)',' ',text)
    text=normalize(text)
    for word,number in (('three','3'),('trois','3'),('vingt','20')):
        text=re.sub(r'\b'+word+r'\b',number,text)
    return re.sub(r'\bpm\b','p m',text)


RULES = {
'outgoing': (
 {'blue_document':r'document.{0,35}(?:bleu|bleue)', 'ready':r'\bpret\w*\b',
  'two_tickets':r'\b2\b.{0,20}(?:billets|tickets)', 'friday':r'\bvendredi\b'},
 {'meeting':r'\breunion\b',
  'do_not_cancel':r'(?:n annulez pas|ne pas annuler|ne devez pas annuler|ne faut pas annuler|ne doit pas etre annulee|ne devrait pas etre annulee)',
  '9_not_10':r'\b9\b.{0,45}(?:pas|non|plutot que).{0,25}\b10\b'},
 {'delivery':r'\blivraison\b','tuesday':r'\bmardi\b','27_euros':r'\b27\b.{0,15}euros?'},
 {'third_order':r'(?:3\s*e?\s*commande|troisieme commande)',
  'keep':r'\b(?:conservez|garder|gardez|conserver)\b',
  'yellow_folder':r'(?:dossier|chemise).{0,25}jaune',
  'send_five_copies':r'(?:envoy|envoi)\w*.{0,35}\b5\b.{0,30}(?:copies|exemplaires)',
  'signed_document':r'document.{0,20}signe', 'reception':r'\b(?:accueil|reception)\b',
  'tomorrow_morning':r'demain.{0,20}matin'},
 {'train_14_30':r'train.{0,50}(?:\b14\b.{0,15}\b30\b|\b2\b.{0,15}\b30\b.{0,25}apres midi)',
  'six_passengers':r'\b6\b.{0,20}(?:passagers|voyageurs)'},
 {'confirm':r'\bconfirm\w*','18_not_80':r'\b18\b.{0,40}(?:pas|non).{0,25}\b80\b'},
 {'last_message':r'dernier message','thursday_appointment':r'(?:rendez vous|rendezvous).{0,40}jeudi',
  'keep_original':r'(?:gard|conserv)\w*.{0,30}original'},
),
'incoming': (
 {'green_folder':r'green.{0,25}(?:folder|file)','four_pages':r'\b4\b.{0,15}pages',
  'meeting':r'\bmeeting\b','monday':r'\bmonday\b'},
 {'document':r'\bdocument\b','do_not_delete':r'(?:do not|don t|must not|should not|not to).{0,25}(?:delete|remove)',
  '15_not_16':r'(?:\b15\b.{0,40}(?:not|rather than).{0,20}\b16\b|\b3\b.{0,10}p m.{0,25}not.{0,20}\b4\b)'},
 {'delivery':r'\bdelivery\b','wednesday':r'\bwednesday\b','39_euros':r'\b39\b.{0,15}euros?'},
 {'fourth_order':r'(?:4\s*th\s*order|fourth order)', 'keep':r'\b(?:keep|retain|preserve)\b',
  'red_folder':r'red.{0,25}(?:folder|file)',
  'send_seven_copies':r'send.{0,35}\b7\b.{0,35}(?:copies|duplicates)',
  'signed_document':r'signed.{0,20}document','reception':r'\b(?:reception|front desk|welcome desk)\b',
  'tomorrow_afternoon':r'tomorrow.{0,20}afternoon'},
 {'train_16_20':r'train.{0,50}(?:\b16\b.{0,15}\b20\b|\b4\b.{0,10}\b20\b.{0,10}p m)',
  'eight_passengers':r'\b8\b.{0,20}(?:passengers|travelers|travellers)'},
 {'confirm':r'\bconfirm\w*','13_not_30':r'\b13\b.{0,40}(?:not|rather than).{0,25}\b30\b'},
 {'last_message':r'last message','friday_appointment':r'appointment.{0,40}friday',
  'keep_original':r'(?:keep|retain|preserve).{0,30}original'},
)}


def check(direction,fixture,texts):
    variants=[prepared(''.join(texts)),prepared(' '.join(texts))]
    facts={name:any(re.search(pattern,text) for text in variants)
           for name,pattern in RULES[direction][fixture].items()}
    # Keyword presence must not turn an inverted positive instruction into a pass.
    for name in ('keep','keep_original'):
        if name not in facts:continue
        wrong=(r'(?:ne|n) (?:conserv|gard)\w*.{0,10}pas' if direction=='outgoing'
               else r'(?:do not|don t|must not|should not).{0,10}(?:keep|retain|preserve)')
        if any(re.search(wrong,text) for text in variants):facts[name]=False
    return {'expected_facts':facts,'flagged_for_review':[key for key,value in facts.items() if not value],
            'all_reference_facts_matched':all(facts.values()),'general_accuracy_verified':False}
