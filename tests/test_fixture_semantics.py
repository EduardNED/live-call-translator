import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
from fixture_semantics import check


def test_clock_currency_and_word_chunk_formats_are_supported():
    assert check('outgoing',2,['La livraison est mardi. Le prix est de 27€ plaisir.'])['all_reference_facts_matched']
    assert check('outgoing',4,['Le train part à 14h30. Il y a six passagers.'])['all_reference_facts_matched']
    assert check('incoming',4,['The train leaves at 4:20 p.m. There are eight passengers.'])['all_reference_facts_matched']
    assert check('outgoing',1,["N'annulez pas la réunion. Elle commence à 9h, pas 10h."])['all_reference_facts_matched']
    result=check('incoming',3,['For the fourth order, please keep the red folder and send seven',
        'copies of the signed document to the reception desk tomorrow in the afternoon.'])
    assert result['all_reference_facts_matched']


def test_negative_and_numeric_reversal_are_rejected():
    assert not check('outgoing',1,['Annulez la réunion. Dix, pas neuf.'])['all_reference_facts_matched']
    assert not check('incoming',5,['Please confirm the number is thirty, not thirteen.'])['all_reference_facts_matched']
    assert not check('incoming',6,['This is the last message. The appointment is Friday. Do not keep the original.'])['all_reference_facts_matched']


def test_whole_boundary_reference_checks_include_order_signed_document_and_daypart():
    good=check('outgoing',3,['Pour la troisième commande, conservez le dossier jaune et envoyez cinq copies du document signé à la réception demain matin.'])
    assert good['all_reference_facts_matched']
    bad=check('outgoing',3,['Pour la deuxième commande, conservez le dossier jaune et envoyez cinq copies du document à la réception demain soir.'])
    assert {'third_order','signed_document','tomorrow_morning'}<=set(bad['flagged_for_review'])
    assert good['general_accuracy_verified'] is False


def test_spelled_three_pm_and_compact_pm_clock_are_normalized():
    for text in ('Do not delete the document. The appointment is at three p.m., not four p.m.',
                 'Do not delete the document. The appointment is at 3pm, not 4pm.'):
        assert check('incoming',1,[text])['all_reference_facts_matched']
    assert not check('incoming',1,['Do not delete the document. The appointment is at four p.m., not three p.m.'])['all_reference_facts_matched']
