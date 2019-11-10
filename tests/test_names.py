# -*- coding: utf-8 -*-
from .context import names as n
from itertools import repeat
import unittest

class CleanNameTests(unittest.TestCase):
    def testUpercase(self):
        self.assertEqual(n.clean_name('John Doe'), 'JOHN DOE')

    def testRemoveSpaces(self):
        self.assertEqual(n.clean_name(' John  Doe '), 'JOHN DOE')

    def testKeepMiddleName(self):
        self.assertEqual(n.clean_name(' John Freddy  Doe '), 'JOHN FREDDY DOE')

    def testRemoveAccents(self):
        self.assertEqual(n.clean_name('João Silva'), 'JOAO SILVA')

    def testRemoveAccentsAndSpaceKeepE(self):
        self.assertEqual(n.clean_name('João  Silva e  Costa'), 'JOAO SILVA E COSTA')

    def testNone(self):
        self.assertEqual(n.clean_name(None), None)   

    def testEmpty(self):
        self.assertEqual(n.clean_name(''), '')   

    def testBlank(self):
        self.assertEqual(n.clean_name('  '), '')   

class CanonName(unittest.TestCase):
    def testNones(self):
        self.assertEqual(None, n.canon_name(None, None))
    def testOneNone(self):
        self.assertEqual(None, n.canon_name('joão', None))
    def testEqual(self):
        self.assertEqual('JOAO', n.canon_name('joão', 'joão'))
    def testChangeCaps(self):
        self.assertEqual('JOAO', n.canon_name('joão', 'joão'))
    def testAddSpaces(self):
        self.assertEqual('JOAO', n.canon_name(' joão', 'joão  '))
    def testTwoNames(self):
        self.assertEqual('JOAO SILVA', n.canon_name('joão silva', 'João Silva'))
    def testTypoInLastName(self):
        self.assertEqual(None, n.canon_name('joão silva', 'João Silvo'))
    def testTypoInLastNameLevenshtein(self):
        self.assertEqual('JOAO SILVA', n.canon_name('joão silva', 'João Silvo',
                                                    levenshtein=1))
    def testTypoInLastNameLevenshteinLast(self):
        self.assertEqual('JOAO SILVA', n.canon_name('joão silva', 'João Silvo',
                                                    levenshtein_last=1))
        self.assertEqual('LUCAS VIANA KNOCHENHAUER', \
                         n.canon_name('LUCAS VIANA KNOCHENHAUER', \
                                      'Lucas Viana Knochenhuaer', \
                                      levenshtein_last=1))
    def testTypoInLastNameLevenshteinLast0(self):
        self.assertEqual(None, n.canon_name('joão silva', 'João Silvo',
                                            levenshtein=1,
                                            levenshtein_last=0))
    def testTypoInFirstName(self):
        self.assertEqual('JOSE CARLOS', n.canon_name('José Carlos', 'Jose Carlos'))
        self.assertEqual('JOSE CARLOS', n.canon_name('José Carlos', 'Jose Carlos',
                                                     levenshtein=0))
    def testTypoInFirstName(self):
        self.assertEqual(None, n.canon_name('Josi Carlos', 'Jose Carlos',
                                            levenshtein=0))
        self.assertEqual(None, n.canon_name('Josi Carlos', 'Jose Carlos',
                                            levenshtein=0,
                                            levenshtein_last=1))
        self.assertEqual('JOSI CARLOS', n.canon_name('Josi Carlos', 'Jose Carlos',
                                                     levenshtein=1,
                                                     levenshtein_last=0))            
    def testTypoInMiddleName(self):
        ex = 'JOSE VANDER CARLOS'
        self.assertEqual(None, n.canon_name('José Vander Carlos', 'Jose Vinder Carlos',
                                            levenshtein=0))
        self.assertEqual(ex, n.canon_name('José Vander Carlos', 'Jose Vinder Carlos',
                                            levenshtein=1))
        self.assertEqual(ex, n.canon_name('José Vander Carlos', 'Jose Vinder Carlos',
                                            levenshtein=1, levenshtein_last=0))
    def testExtraMiddleName(self):
        ex = 'JOSE VANDER GRESSE CARLOS'
        self.assertEqual(ex, n.canon_name('José Vander Gresse Carlos',
                                          'Jose Vander Carlos',
                                          levenshtein=0))
        self.assertEqual(ex, n.canon_name('José Vander Carlos',
                                          'Jose Vander Gresse Carlos',
                                          levenshtein=0))
        self.assertEqual(ex, n.canon_name('José Vander Gresse Carlos',
                                          'Jose Vander Carlos',
                                          levenshtein=1))
        self.assertEqual(ex, n.canon_name('José Vander Carlos',
                                          'Jose Vander Gresse Carlos',
                                          levenshtein=1))
        self.assertEqual(None, n.canon_name('José Vander Gressi Carlos',
                                            'Jose Vander Gresse Carlos',
                                            levenshtein=0))
        self.assertEqual(None, n.canon_name('José Vander Gressi Carlos',
                                            'Jose Vinder Gresse Carlos',
                                            levenshtein=0))
    def testTypoInLastNameCanonDecideByOrder(self):
        a, b = 'Fulano Silveira', 'Fulano Silvera'
        self.assertEqual(n.clean_name(a), n.canon_name(a, b, levenshtein_last=1))
        self.assertEqual(n.clean_name(b), n.canon_name(b, a, levenshtein_last=1))
    def testAlwaysTolerateDots(self):
        a, b = 'C. Leite', 'C Leite'
        self.assertEqual(n.clean_name(a), n.canon_name(a, b))
    def testNoLevInMiddleName(self):
        a, b = 'C. Leite', 'D. Leite'
        self.assertEqual(None, n.canon_name(a, b, levenshtein=1))
        
class CanonMapsTest(unittest.TestCase):
    def testEmpytLists(self):
        self.assertEqual(list(repeat(dict(), 2)), n.canon_maps([], []))
        self.assertEqual(list(repeat(dict(), 3)), n.canon_maps([], [], []))
    def testSingleList(self):
        self.assertEqual([dict()], n.canon_maps([]))
    def testNoMatches(self):
        a, b, c = ['alice', 'andrew'], ['bob'], ['charlie', 'carol', 'chris']
        self.assertEqual(list(repeat(dict(), 2)), n.canon_maps(a, b))
        self.assertEqual(list(repeat(dict(), 3)), n.canon_maps(a, b, c))
        self.assertEqual(list(repeat(dict(), 1)), n.canon_maps(a))
    def testOnlyExactMatches(self):
        a, b, c = ['alice', 'andrew'], ['bob', 'alice'], ['bob', 'carol']
        self.assertEqual(list(repeat(dict(), 2)), n.canon_maps(a, b))
        self.assertEqual(list(repeat(dict(), 3)), n.canon_maps(a, b, c))
        self.assertEqual(list(repeat(dict(), 1)), n.canon_maps(a))
    def testAccentMatches(self):
        a, b = ['John Doe', 'João Silva'], ['Joao Silva', 'Joana Silva']
        self.assertEqual([dict(), dict()], n.canon_maps(a, b))
    def testMissingMiddleName(self):
        a, b = ['John Doe', 'João Silva'], ['Joao Gomes Silva']
        self.assertEqual([{n.clean_name(a[1]): n.clean_name(b[0])}, dict()], \
                         n.canon_maps(a, b))
    def testMissingMiddleNameWithExactMatch(self):
        a, b, c = ['John Doe', 'João Silva'], ['Joao Gomes Silva'], ['João Silva']
        d = {n.clean_name(a[1]): n.clean_name(b[0])}
        self.assertEqual([d, dict(), d], n.canon_maps(a, b, c))
    def testAmbiguous(self):
        a = ['John Doe', 'João Silva']
        b = ['Joao Gomes Silva', 'João Brito Silva']
        c = ['João Silva']
        self.assertEqual([dict(), dict(), dict()], n.canon_maps(a, b, c))
    def testAmbigousUnderLevenshteinLast(self):
        a = ['Fulano Silveira']
        b = ['Fulano Silveira', 'Fulano Silvera']
        self.assertEqual([dict(), dict()], n.canon_maps(a, b))
    def testAllowAmbiguous(self):
        a = ['Fulano Silveira']
        b = ['Fulano Silveira', 'Fulano Silvera']
        self.assertEqual([dict(), {n.clean_name(b[1]): n.clean_name(a[0])}], \
                         n.canon_maps(a, b, allow_ambiguous=True))
