#include <stdio.h>
#include <limits.h>
#include <string.h>

#include "../document.h"
#include "../misc.h"
#include "../match.h"
#include "../util.h"
#include "../core.h"

static void strpool_dump(struct docent* docent) {
	unsigned int i, j;

	if (docent == NULL) return;

	char** strptr = docent->strpool;
	for (i = 0; i < WORD_LENGTH_RANGE; ++i) {
		if (docent->strents[i].num != 0) {
			printf("---WORD LENTH %d, WORD NUM %u---\n",
			       i+MIN_WORD_LENGTH, docent->strents[i].num);
			for (j = 0; j < docent->strents[i].num; ++j) {
				*(*strptr+i+MIN_WORD_LENGTH) = '\0';
				printf("%s\t", *strptr);
				*(*strptr+i+MIN_WORD_LENGTH) = ' ';
				strptr++;

			}
			printf("\n\n");
		}
	}
}

int main(int argc, char *argv[])
{
	// char* str = "http resoe 55555 dbpea 55551 55553 55552 55554  list women 666666";
	char *str = strdup("http dbpedia resource usair flight http dbpedia ontology abstract usair flight regularly scheduled domestic passenger flight between laguardia airport york city cleveland ohio march usair fokker registration flying route crashed poor weather partially inverted position flushing york shortly after liftoff from laguardia undercarriage lifted from runway however airplane failed gain lift flying only several meters above ground aircraft then veered runway multiple obstructions before coming rest flushing just beyond runway people board were killed accident including captain cabin crew members subsequent investigation revealed that pilot error inadequate deicing procedures laguardia several lengthy delays large amount accumulated wings airframe this disrupted airflow over increasing drag reducing lift which prevented from lifting runway national transportation safety board ntsb concluded that flight crew were unaware amount that built after delayed heavy ground traffic taxiing runway report also listed fact that aircraft begun takeoff rotation early lower speed than standard contributing factor accident investigators also found that deicing procedures laguardia were substandard while encountered delay minutes they found that deicing fluid that being used airdport majority commercial airliees across united states effective only fifteen minutes accident number studies into effect that aircraft several recommendations into prevention techniques http wikipedia wiki usair flight");

	struct docent * docent = docent_new(str);

	/**
	 * Memory Dump Test
	 */
	strpool_dump(docent);

	/**
	 * Match Minimum Distance Test
	 */
	struct document_match doc_match;
	doc_match.docent = docent;
	int dist;

	/* Exact Match Test */
	printf ("---Exact Match Test---\n");
	char* exact_word = "55555";
	dist = match_min_dist(&doc_match, MT_EXACT_MATCH, exact_word, strlen(exact_word), INT_MAX);
	printf ("Exact Match Test, search word %s, dist %d\n", exact_word, dist);
	char* exact_word1 = "55557";
	dist = match_min_dist(&doc_match, MT_EXACT_MATCH, exact_word1, strlen(exact_word1), INT_MAX);
	printf ("Exact Match Test, search word %s, dist %d\n", exact_word1, dist);
	printf ("\n");

	/* Hamming Distance Match Test */
	printf ("---Hamming Distance Match Test---\n");
	char* hamming_word = "55555";
	dist = match_min_dist(&doc_match, MT_HAMMING_DIST, hamming_word, strlen(hamming_word), INT_MAX);
	printf ("Hamming Distance Match Test, search word %s, dist %d\n", hamming_word, dist);
	char* hamming_word1 = "55557";
	dist = match_min_dist(&doc_match, MT_HAMMING_DIST, hamming_word1, strlen(hamming_word1), INT_MAX);
	printf ("Hamming Distance Match Test, search word %s, dist %d\n", hamming_word1, dist);

    const char *hamming_word2 = "pgdma";
    dist = match_min_dist(&doc_match, MT_HAMMING_DIST, hamming_word2, strlen(hamming_word2), INT_MAX);
    printf ("Hamming Distance Match Test, search word %s, dist %d\n", hamming_word2, dist);

    printf ("\n");

	/* Edit Distance Match Test */
	printf ("---Edit Distance Match Test---\n");
	char* edit_word = "55555";
	dist = match_min_dist(&doc_match, MT_EDIT_DIST, edit_word, strlen(edit_word), INT_MAX);
	printf ("Edit Distance Match Test, search word %s, dist %d\n", edit_word, dist);
	char* edit_word1 = "5";
	dist = match_min_dist(&doc_match, MT_EDIT_DIST, edit_word1, strlen(edit_word1), INT_MAX);
	printf ("Edit Distance Match Test, search word %s, dist %d\n", edit_word1, dist);

	docent_destroy(docent);

	free(str);
	return 0;
}
